package com.ankurdave.toynoria

import scala.collection.mutable

case class Table[T <: Record]() extends UnaryNode[T, T] with FullStateNode[T] {
  private val records = new mutable.HashMap[Id, T]

  override def handle(msg: Msg[T]): Unit = {
    logTrace("Table.handle " + msg)
    msg match {
      case Insert(x) =>
        records.update(x.id, x)
      case Update(x) =>
        records.update(x.id, x)
      case Delete(x) =>
        records -= x.id
      case Evict(x) =>
        // Do nothing: The base table must not evict records, but parents may
    }
    sendToParents(msg)
  }

  override def query(): Seq[T] = records.values.toSeq
}

case class Aggregate[A <: Record](
  zero: (Id) => A,
  add: (A, A) => A,
  subtract: (A, A) => A,
  child: Node[A]) extends UnaryNode[A, A] {

  private val state = new mutable.HashMap[Id, A]
  private var partialStateEnabled = true

  override def disablePartialState(): Unit = {
    partialStateEnabled = false
  }

  override def handle(msg: Msg[A]): Unit = {
    logTrace("Aggregate.handle " + msg)
    logTrace("pre: Agg = " + state.toString)
    msg match {
      case Insert(a) =>
        if (state.contains(a.id)) {
          state.update(a.id, add(state(a.id), a))
          sendToParents(Update(state(a.id)))
        } else {
          // id might have been evicted, so request all matching records from child
          state.update(a.id, zero(a.id))
          sendToParents(Insert(state(a.id)))
          logTrace("Aggregate requesting all records...")
          for {
            a2 <- child.query()
            if a.id == a2.id
          } {
            handle(Insert(a2))
          }
          logTrace("...done.")
        }

      case Update(a) =>
        if (state.contains(a.id)) {
          state.update(a.id, add(state(a.id), a))
          sendToParents(Update(state(a.id)))
        } else {
          // id must have been evicted, so silently drop the update
        }

      case Delete(a) =>
        if (state.contains(a.id)) {
          state.update(a.id, subtract(state(a.id), a))
          // Send parents an update, not a delete, because deletion from an aggregation is just a
          // subtraction
          sendToParents(Update(state(a.id)))
        } else {
          // id must have been evicted, so silently drop the deletion
        }

      case Evict(a) =>
        if (partialStateEnabled) {
          if (state.contains(a.id)) {
            state -= a.id
            sendToParents(Evict(a))
          }
        }
    }
    logTrace("post: Agg = " + state.toString)
  }

  override def query(): Seq[A] = {
    state.values.toSeq
  }
}

case class Join[A <: Record, B <: Record, C <: Record](
  combine: (A, B) => C,
  left: Node[A],
  right: Node[B]) extends BinaryNode[A, B, C] with StatelessNode[C] {

  override def query(): Seq[C] = {
    val as = left.query().groupBy(_.id)
    val bs = right.query().groupBy(_.id)
    for {
      id <- as.keySet.intersect(bs.keySet).toSeq
      a <- as(id)
      b <- bs(id)
    } yield combine(a, b)
  }

  override def handleLeft(msg: Msg[A]): Unit = {
    logTrace("Join.handleLeft " + msg)
    msg match {
      case Insert(a) =>
        for (b <- right.query(); if a.id == b.id) {
          sendToParents(Insert(combine(a, b)))
        }

      case Update(a) =>
        for (b <- right.query(); if a.id == b.id) {
          sendToParents(Update(combine(a, b)))
        }

      case Delete(a) =>
        for (b <- right.query(); if a.id == b.id) {
          sendToParents(Delete(combine(a, b)))
        }

      case Evict(a) =>
        // Join is stateless, so no need to do anything on eviction
    }
  }

  override def handleRight(msg: Msg[B]): Unit = {
    logTrace("Join.handleRight " + msg)
    msg match {
      case Insert(b) =>
        for (a <- left.query(); if a.id == b.id) {
          sendToParents(Insert(combine(a, b)))
        }

      case Update(b) =>
        for (a <- left.query(); if a.id == b.id) {
          sendToParents(Update(combine(a, b)))
        }

      case Delete(b) =>
        for (a <- left.query(); if a.id == b.id) {
          sendToParents(Delete(combine(a, b)))
        }

      case Evict(b) =>
        // Join is stateless, so no need to do anything on eviction
    }
  }
}

case class TopK[A <: Record : Ordering](
  k: Int,
  child: Node[A]) extends UnaryNode[A, A] with FullStateNode[A] {

  private val state = mutable.HashMap[Id, A]()

  override def query(): Seq[A] = {
    state.values.toSeq.sorted
  }

  override def handle(msg: Msg[A]): Unit = {
    logTrace("TopK.handle " + msg)
    logTrace("pre: TopK = " + state.toString)
    msg match {
      case Insert(a) =>
        if (state.size < k) {
          state.update(a.id, a)
          sendToParents(Insert(a))
        } else if (implicitly[Ordering[A]].gt(a, state.values.min)) {
          val dropped = state.values.min
          state.remove(dropped.id)
          sendToParents(Delete(dropped))
          state.update(a.id, a)
          sendToParents(Insert(a))
        }

      case Update(a) =>
        if (implicitly[Ordering[A]].gt(a, state.values.min)) {
          // The element is in the top k after the update
          if (state.contains(a.id)) {
            state.update(a.id, a)
            sendToParents(Update(a))
          } else {
            handle(Insert(a))
          }
        } else {
          if (state.contains(a.id)) {
            // The update has caused this record to drop below the current minimum. It may still be
            // in the top k, or some other element may now replace it in the top k. For correctness
            // we must request a full refresh from the child.
            logTrace("clear")
            for (a2 <- query()) {
              sendToParents(Delete(a2))
            }
            state.clear()
            for (a2 <- child.query()) {
              handle(Insert(a2))
            }
          }
        }

      case Delete(a) =>
        if (state.contains(a.id)) {
          state -= a.id
          sendToParents(Delete(a))

          // After the deletion, some other element may now replace the deleted element in the top
          // k. For correctness we must request a full refresh from the child.
          logTrace("clear")
          for (a2 <- query()) {
            sendToParents(Delete(a2))
          }
          state.clear()
          for (a2 <- child.query()) {
            handle(Insert(a2))
          }
        }

      case Evict(x) =>
        // TopK does not support eviction because it inherently uses bounded space. Drop the
        // eviction request.
    }
    logTrace("post: TopK = " + state.toString)

  }
}
