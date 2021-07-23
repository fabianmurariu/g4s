package com.github.fabianmurariu.g4s.optim.impls

import com.github.fabianmurariu.g4s.sparse.grbv2.GrBMatrix
import com.github.fabianmurariu.g4s.sparse.grbv2.MxM
import com.github.fabianmurariu.g4s.sparse.grb.GrBSemiring
import cats.effect.Sync
import cats.implicits._
import com.github.fabianmurariu.g4s.matrix.BlockingMatrix
import com.github.fabianmurariu.g4s.sparse.grb.GRB
import com.github.fabianmurariu.g4s.optim.Name
import com.github.fabianmurariu.g4s.optim.LogicMemoRef
import cats.Monad
import cats.effect.IO
import com.vladsch.flexmark.util.ast.Block
import zio.ZIO
import cats.data.StateT

/**
  * base class for push operators
  * inspired by "How to Architect a Query Compiler, Revised" Ruby T Tahboub, et al.
  */
trait Operator[F[_]] {

  def eval(cb: Record => F[Unit]): F[Unit]

  /**
    * The binding on which the operator output is sorted
    *
    * @return
    */
  def sorted: Option[Name]

  /**
    * What bindings are covered by this operator
    * [A]
    *
    * or [A, B]
    *
    * @return
    */
  def cover: Set[Name]

  /**
    * the estimated cardinality of the output
    * how much stuff do we expect to produce
    */
  def cardinality: Long
}

/**
  * A placeholder operator
  * pointing to the memo
  * entry of the actual phisical
  * operator
  * */
case class RefOperator[F[_]: Monad](logic: LogicMemoRef[F]) extends Operator[F] {

  override def eval(cb: Record => F[Unit]): F[Unit] = Monad[F].unit

  override def sorted: Option[Name] = logic.sorted

  override def cover: Set[Name] = logic.output

  override def cardinality: Long = -1

}

sealed trait EdgeMatrix[F[_]] extends Operator[F]

case class GetNodeMatrix[F[_]](
    binding: Name,
    name: Option[String],
    nodes: BlockingMatrix[F, Boolean],
    cardinality: Long // very likely nodes.nvals
) extends Operator[F] {

  override def eval(cb: Record => F[Unit]): F[Unit] =
    cb(Nodes(nodes))

  override def sorted: Option[Name] = Some(binding)

  override def cover: Set[Name] = Set(binding)

}

case class GetEdgeMatrix[F[_]](
    binding: Name,
    name: Option[String],
    edges: BlockingMatrix[F, Boolean],
    cardinality: Long // very likely edges.nvals
) extends EdgeMatrix[F] {

  override def eval(cb: Record => F[Unit]): F[Unit] =
    cb(Edges(edges))

  override def sorted: Option[Name] = Some(binding)

  override def cover: Set[Name] = Set(binding)

}

case class MatrixMul[F[_]](
    frontier: Operator[F],
    edges: Operator[F],
)(implicit F: Sync[F], G: GRB)
    extends Operator[F] {

  val semiRing = Expand.staticAnyPairSemiring

  override def eval(cb: Record => F[Unit]): F[Unit] = {
    frontier.eval {
      case left: MatrixRecord[F] =>
        edges.eval {
          case right: MatrixRecord[F] =>
            F.bracket(F.delay(right.mat)) { edges =>
              MxM[F]
                .mxm(left.mat)(left.mat, edges)(semiRing)
                .flatMap(rec => cb(MatrixRecord(rec)))
            }(_.release)
          case e: Edges[F] =>
            e.mat
              .use[GrBMatrix[F, Boolean]] { edges =>
                MxM[F].mxm(left.mat)(left.mat, edges)(semiRing)
              }
              .flatMap(mat => cb(MatrixRecord(mat)))
        }
      case left: Nodes[F] =>
        edges.eval {
          case right: MatrixRecord[F] =>
            left.mat.use { nodes =>
              MxM[F]
                .mxm(right.mat)(nodes, right.mat)(semiRing)
                .flatMap(rec => cb(MatrixRecord(rec)))
            }
          case e: Edges[F] =>
            e.mat
              .use[GrBMatrix[F, Boolean]] { edges =>
                left.mat.use { nodes =>
                  for {
                    shape <- edges.shape
                    (rows, cols) = shape
                    out <- GrBMatrix.unsafe[F, Boolean](rows, cols)
                    output <- MxM[F].mxm(out)(nodes, edges)(semiRing)
                  } yield output
                }
              }
              .flatMap(rec => cb(MatrixRecord(rec)))
        }
    }
  }

  override def sorted: Option[Name] = frontier.sorted

  override def cover: Set[Name] = frontier.cover

  override def cardinality: Long = frontier.cardinality * edges.cardinality

}

object Expand {
  import com.github.fabianmurariu.g4s.sparse.grb.GRB.async.grb
  /**
   * Acquire a single ANY_PAIR semiring and use it
   * it should have no state thus no reason
   * to cause undefined behaviour when
   * shared between threads
   * */
  lazy val staticAnyPairSemiring: GrBSemiring[Boolean, Boolean, Boolean] = {
    val (semi, shutdown) = GrBSemiring.anyPair[IO].allocated.unsafeRunSync()
    Runtime
      .getRuntime()
      .addShutdownHook(new Thread() {
        override def run: Unit = {
          shutdown.unsafeRunSync()
        }
      })
    semi
  }
}
