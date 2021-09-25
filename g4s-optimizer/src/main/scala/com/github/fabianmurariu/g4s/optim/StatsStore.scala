package com.github.fabianmurariu.g4s.optim

import cats.effect.IO
import com.github.fabianmurariu.g4s.optim.MutableGraph
import com.github.fabianmurariu.g4s.optim.DirectedGraph
import cats.effect.kernel.MonadCancel
import cats.effect.std.Queue

sealed trait Label
// case object AllNodes extends Label
// case object AllEdges extends Label
case class NodeLabel(v: String) extends Label
case class EdgeLabel(v: String) extends Label

trait StatsStore {

  def addNode(label: String): IO[StatsStore]

  def addTriplet(
      srcLabel: String,
      edgeLabel: String,
      dstLabel: String
  ): IO[StatsStore]

  def removeNode(label: String): IO[StatsStore]

  def store: IO[MutableGraph[Label, Long]]

  def removeEdge(srcLabel: String, edgeLabel: String): IO[StatsStore]

  /**
    * nodes with [label] / nodesTotal
    * or 1 if label is None
    * */
  def nodeSel(label: Option[String]): IO[Double]

  /**
    * edges with [label] / edgesTotal
    * or 1 if label is None
    * */
  def edgeSel(label: Option[String]): IO[Double]

  /**
    * all the nodes inserted given
    * the [label]
    * */
  def nodesTotal(label: Option[String]): IO[Long]

  /**
    * all the edges inserted given
    * the [label]
    * */
  def edgesTotal(label: Option[String]): IO[Long]

  /**
    *
    * number of [srcLabel] -[edgeLabel]->
    *
    * */
  def nodeEdgeOut(srcLabel: String, edgeLabel: String): IO[Long]

  /**
    *
    * selectivity of [srcLabel] -[edgeLabel]->
    *
    * */
  def nodeEdgeOutSel(srcLabel: String, edgeLabel: String): IO[Double]

  /**
    *
    * number of -[edgeLabel]-> (dstLabel)
    *
    * */
  def nodeEdgeIn(edgeLabel: String, dstLabel: String): IO[Long]

  /**
    *
    * selectivity of -[edgeLabel]-> (dstLabel)
    *
    * */
  def nodeEdgeInSel(edgeLabel: String, dstLabel: String): IO[Double]
}

object StatsStore {

  import DirectedGraph.ops._
  import scala.collection.mutable

  case class StatsData(
      linkCounts: MutableGraph[Label, Long],
      nodes: mutable.HashMap[String, Long],
      edges: mutable.HashMap[String, Long],
      var nodeCount: Long = 0,
      var edgeCount: Long = 0
  )

  def apply(): IO[StatsStore] = {

    def increment(i: Option[Long]): Option[Long] =
      i.map(_ + 1L).orElse(Some(1L))

    val q = for {
      sync <- Queue.bounded[IO, StatsData](1)
      _ <- sync.offer(
        StatsData(
          MutableGraph.empty[Label, Long],
          mutable.HashMap.empty,
          mutable.HashMap.empty
        )
      )
    } yield sync

    q.map { sync =>
      new StatsStore { self =>

        override def store: IO[MutableGraph[Label, Long]] = use { sd =>
          sd.linkCounts
        }

        def useIO[B](fg: StatsData => IO[B]): IO[B] =
          MonadCancel[IO].bracket(sync.take)(fg)(sync.offer _)

        def use[B](fg: StatsData => B): IO[B] =
          useIO({ graph => IO.delay(fg(graph)) })

        def useSelf[B](fg: StatsData => B): IO[StatsStore] =
          use(fg).map(_ => self)

        override def addNode(label: String): IO[StatsStore] =
          useSelf { sd =>
            sd.nodeCount += 1L
            sd.nodes.updateWith(label)(increment)
          }

        override def addTriplet(
            srcLabel: String,
            edgeLabel: String,
            dstLabel: String
        ): IO[StatsStore] =
          useSelf { sd =>
            sd.edgeCount += 1L
            val sl = NodeLabel(srcLabel)
            val el = EdgeLabel(edgeLabel)
            val dl = NodeLabel(dstLabel)

            sd.edges.updateWith(edgeLabel)(increment)

            val countIn = sd.linkCounts.getEdge(sl, el) match {
              case Some((_, v, _)) => v + 1L
              case None            => 1L
            }

            val countOut = sd.linkCounts.getEdge(el, dl) match {
              case Some((_, v, _)) => v + 1L
              case None            => 1L
            }

            sd.linkCounts
              .insert(sl)
              .insert(el)
              .insert(dl)
              .edge(sl, countIn, el)
              .edge(el, countOut, dl)
          }

        override def removeNode(label: String): IO[StatsStore] = ???

        override def removeEdge(
            srcLabel: String,
            edgeLabel: String
        ): IO[StatsStore] = ???

        override def nodeSel(label: Option[String]): IO[Double] =
          for {
            n1 <- nodesTotal(label)
            nTotal <- nodesTotal(None)
          } yield (n1.toDouble / nTotal.toDouble)

        override def edgeSel(label: Option[String]): IO[Double] =
          for {
            n1 <- edgesTotal(label)
            nTotal <- edgesTotal(None)
          } yield (n1.toDouble / nTotal.toDouble)

        override def nodesTotal(label: Option[String] = None): IO[Long] =
          use { sd =>
            label match {
              case Some(nodeLabel) =>
                sd.nodes.get(nodeLabel).getOrElse(0L)
              case None =>
                sd.nodeCount
            }
          }

        override def edgesTotal(label: Option[String]): IO[Long] =
          use { sd =>
            label match {
              case Some(edgeLabel) =>
                sd.edges.get(edgeLabel).getOrElse(0L)
              case None =>
                sd.edgeCount
            }
          }

        override def nodeEdgeOut(
            srcLabel: String,
            edgeLabel: String
        ): IO[Long] = use { sd =>
          sd.linkCounts
            .getEdge(NodeLabel(srcLabel), EdgeLabel(edgeLabel))
            .map(_._2) // get the count
            .getOrElse(0L)
        }

        override def nodeEdgeOutSel(
            srcLabel: String,
            edgeLabel: String
        ): IO[Double] =
          for {
            totalEdge <- nodeEdgeOut(srcLabel, edgeLabel)
            total <- use { st =>
              st.linkCounts.out(NodeLabel(srcLabel)).foldLeft(0L) {
                case (sum, e) => sum + e._2
              }
            }
          } yield totalEdge.toDouble / total.toDouble

        override def nodeEdgeInSel(
            edgeLabel: String,
            dstLabel: String
        ): IO[Double] =
          for {
            totalEdge <- nodeEdgeIn(edgeLabel, dstLabel)
            total <- use(_.linkCounts.in(NodeLabel(dstLabel)).foldLeft(0L) {
                case (sum, e) => sum + e._2
              })
          } yield totalEdge.toDouble / total.toDouble

        override def nodeEdgeIn(
            edgeLabel: String,
            dstLabel: String
        ): IO[Long] = use { sd =>
          sd.linkCounts
            .getEdge(EdgeLabel(edgeLabel), NodeLabel(dstLabel))
            .map(_._2) // get the count
            .getOrElse(0L)

        }

      }
    }

  }

}
