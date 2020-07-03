package com.github.fabianmurariu.g4s.graph

import com.github.fabianmurariu.g4s.sparse.grb.GrBMatrix
import cats.Foldable
import cats.Eval

package object core {
  
  type AdjacencyMap[V, E] = Map[V, Map[V, E]]

  implicit val traverseIsFoldable:Foldable[Traversable] = new Foldable[Traversable]{

    override def foldLeft[A, B](fa: Traversable[A], b: B)(f: (B, A) => B): B =
      fa.foldLeft(b)(f)

    override def foldRight[A, B](fa: Traversable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = 
      fa.foldRight(lb)(f)

  }
}
