package com.github.fabianmurariu.g4s.optim

import com.github.fabianmurariu.g4s.optim.MutableGraph
import com.github.fabianmurariu.g4s.optim.DirectedGraph
import com.rits.cloning.Cloner

sealed trait Label
// case object AllNodes extends Label
// case object AllEdges extends Label
case class NodeLabel(v: String) extends Label
case class EdgeLabel(v: String) extends Label

trait StatsStore {

  def copy: StatsStore

  def addNode(label: String): StatsStore

  def addEdge(label: String): StatsStore

  def addTriplet(
      srcLabel: String,
      edgeLabel: String,
      dstLabel: String
  ): StatsStore

  def removeNode(label: String): StatsStore

  def store: MutableGraph[Label, Long]

  def removeEdge(srcLabel: String, edgeLabel: String): StatsStore

  /**
    * nodes with [label] / nodesTotal
    * or 1 if label is None
    * */
  def nodeSel(label: Option[String]): Double

  /**
    * edges with [label] / edgesTotal
    * or 1 if label is None
    * */
  def edgeSel(label: Option[String]): Double

  /**
    * all the nodes inserted given
    * the [label]
    * */
  def nodesTotal(label: Option[String]): Long

  /**
    * all the edges inserted given
    * the [label]
    * */
  def edgesTotal(label: Option[String]): Long

  /**
    *
    * number of [srcLabel] -[edgeLabel]->
    *
    * */
  def nodeEdgeOut(srcLabel: String, edgeLabel: String): Long

  /**
    *
    * selectivity of [srcLabel] -[edgeLabel]->
    *
    * */
  def nodeEdgeOutSel(srcLabel: String, edgeLabel: String): Double

  /**
    *
    * number of -[edgeLabel]-> (dstLabel)
    *
    * */
  def nodeEdgeIn(edgeLabel: String, dstLabel: String): Long

  /**
    *
    * selectivity of -[edgeLabel]-> (dstLabel)
    *
    * */
  def nodeEdgeInSel(edgeLabel: String, dstLabel: String): Double
}

import scala.collection.mutable

object StatsStore {

  def apply(): StatsStore = {

    val sd = StatsData(
      MutableGraph.empty[Label, Long],
      mutable.HashMap.empty,
      mutable.HashMap.empty
    )

    new NaiveStatsStore(sd)
  }

}

case class StatsData(
    linkCounts: MutableGraph[Label, Long],
    nodes: mutable.HashMap[String, Long],
    edges: mutable.HashMap[String, Long],
    var nodeCount: Long = 0,
    var edgeCount: Long = 0
)

class NaiveStatsStore(sd: StatsData) extends StatsStore { self =>

  private val cloner = new Cloner

  import DirectedGraph.ops._

  def increment(i: Option[Long]): Option[Long] =
    i.map(_ + 1L).orElse(Some(1L))
  def copy: StatsStore = new NaiveStatsStore(cloner.deepClone(sd)) // FIXME: huge BUG this is a shallow clone

  override def store: MutableGraph[Label, Long] = use { sd => sd.linkCounts }

  def use[B](fg: StatsData => B): B = fg(sd)

  def useSelf[B](fg: StatsData => B): StatsStore = {
    use(fg)
    self
  }

  override def addNode(label: String): StatsStore =
    useSelf { sd =>
      sd.nodeCount += 1L
      sd.nodes.updateWith(label)(increment)
    }

  override def addEdge(label: String): StatsStore =
    useSelf { sd =>
      sd.edgeCount += 1L
      sd.edges.updateWith(label)(increment)
    }
  override def addTriplet(
      srcLabel: String,
      edgeLabel: String,
      dstLabel: String
  ): StatsStore =
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

  override def removeNode(label: String): StatsStore = ???

  override def removeEdge(
      srcLabel: String,
      edgeLabel: String
  ): StatsStore = ???

  override def nodeSel(label: Option[String]): Double = {
    val n1 = nodesTotal(label)
    val nTotal = nodesTotal(None)
    (n1.toDouble / nTotal.toDouble)
  }

  override def edgeSel(label: Option[String]): Double = {
    val n1 = edgesTotal(label)
    val nTotal = edgesTotal(None)
    (n1.toDouble / nTotal.toDouble)
  }
  override def nodesTotal(label: Option[String] = None): Long =
    use { sd =>
      label match {
        case Some(nodeLabel) =>
          sd.nodes.get(nodeLabel).getOrElse(0L)
        case None =>
          sd.nodeCount
      }
    }

  override def edgesTotal(label: Option[String]): Long =
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
  ): Long = use { sd =>
    sd.linkCounts
      .getEdge(NodeLabel(srcLabel), EdgeLabel(edgeLabel))
      .map(_._2) // get the count
      .getOrElse(0L)
  }

  override def nodeEdgeOutSel(
      srcLabel: String,
      edgeLabel: String
  ): Double = {
    val totalEdge = nodeEdgeOut(srcLabel, edgeLabel)
    val total = use { st =>
      st.linkCounts.out(NodeLabel(srcLabel)).foldLeft(0L) {
        case (sum, e) => sum + e._2
      }
    }
    totalEdge.toDouble / total.toDouble
  }

  override def nodeEdgeInSel(
      edgeLabel: String,
      dstLabel: String
  ): Double = {
    val totalEdge = nodeEdgeIn(edgeLabel, dstLabel)
    val total = use(_.linkCounts.in(NodeLabel(dstLabel)).foldLeft(0L) {
      case (sum, e) => sum + e._2
    })
    totalEdge.toDouble / total.toDouble
  }

  override def nodeEdgeIn(
      edgeLabel: String,
      dstLabel: String
  ): Long = use { sd =>
    sd.linkCounts
      .getEdge(EdgeLabel(edgeLabel), NodeLabel(dstLabel))
      .map(_._2) // get the count
      .getOrElse(0L)

  }

}
