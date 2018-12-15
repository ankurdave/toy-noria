package com.ankurdave.toynoria

import scala.collection.mutable

/**
 * A record that exposes a key.
 * 
 * The key is used for different purposes by the various operators. Most commonly it enables
 * multiple updates to the same record to be grouped together (e.g., in [[Table]] and [[TopK]]).
 * 
 * It also enables grouping for [[Join]] and [[Aggregate]]. In this case, the user must arrange for
 * the input type to the join or aggregation to expose the correct field as the id.
 */
trait Record {
  def id: Id
}

/**
 * A Noria dataflow node.
 * 
 * Each node maintains references to its children and its parents. By convention, a node's children
 * are passed in through its constructor. Its parents are set afterwards using [[addParent]],
 * [[addLeftParent]], and [[addRightParent]].
 * 
 * A node receives input records ([[Msg]]s) from its children, transforms the input according to its
 * operator semantics, and passes the output to its parents.
 * 
 * A node can also be queried directly using [[query]]. This is called an upquery in the Noria
 * paper.
 * 
 * All message-passing operations between nodes are currently synchronous and single-threaded.
 * 
 * `ResultType` represents the output type of a node.
 */
trait Node[ResultType <: Record] {
  /** Requests a full scan of this node's output. */
  def query(): Seq[ResultType]

  /** Requests a filtered scan of this node's output. */
  def query(id: Id): Seq[ResultType]

  /** Requests the set of keys represented in this node's output. */
  def keySet(): Set[Id]

  private val parents = mutable.ArrayBuffer.empty[UnaryNode[ResultType, _]]
  private val leftParents = mutable.ArrayBuffer.empty[BinaryNode[ResultType, _, _]]
  private val rightParents = mutable.ArrayBuffer.empty[BinaryNode[_, ResultType, _]]

  /** 
   * Registers a parent of this node. The parent must accept this node's output as its only input
   * (i.e., it must be a [[UnaryNode]] with `InputType` equal to this node's `ResultType`).
   */
  def addParent(p: UnaryNode[ResultType, _]): Unit = {
    parents += p
  }

  /** 
   * Registers a parent of this node. The parent must accept this node's output as its left input
   * (i.e., it must be a [[BinaryNode]] with `LeftInputType` equal to this node's `ResultType`).
   */
  def addLeftParent(p: BinaryNode[ResultType, _, _]): Unit = {
    leftParents += p
  }

  /** 
   * Registers a parent of this node. The parent must accept this node's output as its right input
   * (i.e., it must be a [[BinaryNode]] with `RightInputType` equal to this node's `ResultType`).
   */
  def addRightParent(p: BinaryNode[_, ResultType, _]): Unit = {
    rightParents += p
  }

  def clearParents(): Unit = {
    parents.clear()
    leftParents.clear()
    rightParents.clear()
  }

  /** Sends output to all this node's parents. Currently implemented synchronously. */
  protected def sendToParents(x: Msg[ResultType]): Unit = {
    for (p <- parents) {
      p.handle(x)
    }
    for (p <- leftParents) {
      p.handleLeft(x)
    }
    for (p <- rightParents) {
      p.handleRight(x)
    }
  }

  /** 
   * Disables partial materialization. 
   * 
   * Per Section 4.5 of the Noria paper, this must be called on all operators upstream of a
   * [[FullStateNode]]. This is because a [[FullStateNode]] cannot correctly handle evictions.
   */
  def disablePartialState(): Unit
}

/** A Noria dataflow node with one child. */
trait UnaryNode[InputType <: Record, ResultType <: Record] extends Node[ResultType] {
  /** Receives input from the child node. */
  def handle(msg: Msg[InputType]): Unit
}

/** A Noria dataflow node with two children. */
trait BinaryNode[LeftInputType <: Record, RightInputType <: Record, ResultType <: Record]
  extends Node[ResultType] {

  /** Receives input from the left child node. */
  def handleLeft(msg: Msg[LeftInputType]): Unit

  /** Receives input from the right child node. */
  def handleRight(msg: Msg[RightInputType]): Unit
}

/** 
 * A mixin trait for a stateless Noria dataflow node. Stateless nodes upstream of a
 * [[FullStateNode]] do not need to change their behavior when [[disablePartialState]] is called,
 * because they have no state.
 */
trait StatelessNode[ResultType <: Record] extends Node[ResultType] {
  override def disablePartialState(): Unit = {}
}

/** 
 * A mixin trait for a full-state Noria dataflow node that cannot handle evictions.
 * 
 * The user must call [[disablePartialState]] on all nodes upstream of a [[FullStateNode]].
 * Full-state nodes upstream of a [[FullStateNode]] do not need to change their behavior when
 * [[disablePartialState]] is called, because they do not support partial state.
 */
trait FullStateNode[ResultType <: Record] extends Node[ResultType] {
  override def disablePartialState(): Unit = {}
}
