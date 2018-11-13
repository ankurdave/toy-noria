package com.ankurdave.toynoria

import scala.collection.mutable

trait Record {
  def id: Id
}

/**
 * Noria dataflow node.
 */
trait Node[ResultType <: Record] {
  def query(): Seq[ResultType]

  private val parents = mutable.ArrayBuffer.empty[UnaryNode[ResultType, _]]
  private val leftParents = mutable.ArrayBuffer.empty[BinaryNode[ResultType, _, _]]
  private val rightParents = mutable.ArrayBuffer.empty[BinaryNode[_, ResultType, _]]
  def addParent(p: UnaryNode[ResultType, _]): Unit = {
    parents += p
  }
  def addLeftParent(p: BinaryNode[ResultType, _, _]): Unit = {
    leftParents += p
  }
  def addRightParent(p: BinaryNode[_, ResultType, _]): Unit = {
    rightParents += p
  }
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

  /** Sec 4.5: Noria disables partial state for operators upstream of full-state descendants */
  def disablePartialState(): Unit
}

trait UnaryNode[InputType <: Record, ResultType <: Record] extends Node[ResultType] {
  def handle(msg: Msg[InputType]): Unit
}

trait BinaryNode[LeftInputType <: Record, RightInputType <: Record, ResultType <: Record]
  extends Node[ResultType] {

  def handleLeft(msg: Msg[LeftInputType]): Unit
  def handleRight(msg: Msg[RightInputType]): Unit
}

trait StatelessNode[ResultType <: Record] extends Node[ResultType] {
  override def disablePartialState(): Unit = {}
}

trait FullStateNode[ResultType <: Record] extends Node[ResultType] {
  override def disablePartialState(): Unit = {}
}
