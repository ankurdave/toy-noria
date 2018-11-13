package com.ankurdave.toynoria

import scala.collection.mutable

/**
 * Noria dataflow node.
 */
sealed trait Node[ResultType] {
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
}

sealed trait UnaryNode[InputType, ResultType] extends Node[ResultType] {
  def handle(msg: Msg[InputType]): Unit
}

sealed trait BinaryNode[LeftInputType, RightInputType, ResultType] extends Node[ResultType] {
  def handleLeft(msg: Msg[LeftInputType]): Unit
  def handleRight(msg: Msg[RightInputType]): Unit
}

case class Table[T]() extends UnaryNode[T, T] {
  private val records = new mutable.HashSet[T]

  override def handle(msg: Msg[T]): Unit = {
    println("Table.handle " + msg)
    msg match {
      case Insert(x) =>
        records += x
      case Update(x) =>
        records -= x
        records += x
      case Delete(x) =>
        records -= x
      case Evict(x) =>
        // Do nothing: The base table must not evict records, but parents may
    }
    sendToParents(msg)
  }

  override def query(): Seq[T] = records.toSeq
}

case class Aggregate[A, B](
  key: A => (Id, B),
  add: (B, B) => B,
  subtract: (B, B) => B,
  child: Node[A]) extends UnaryNode[A, (Id, B)] {

  private val state = new mutable.HashMap[Id, B]

  override def handle(msg: Msg[A]): Unit = {
    println("Aggregate.handle " + msg)
    msg match {
      case Insert(x) =>
        val (id, b) = key(x)
        if (state.contains(id)) {
          state.update(id, add(state(id), b))
          sendToParents(Update(Tuple2(id, state(id))))
        } else {
          state.update(id, b)
          sendToParents(Insert(Tuple2(id, state(id))))
        }

      case Update(x) =>
        val (id, b) = key(x)
        if (state.contains(id)) {
          state.update(id, add(state(id), b))
          sendToParents(Update(Tuple2(id, state(id))))
        } else {
          // id must have been evicted, so silently drop the update
        }

      case Delete(x) =>
        val (id, b) = key(x)
        if (state.contains(id)) {
          state.update(id, subtract(state(id), b))
          sendToParents(Update(Tuple2(id, state(id))))
        } else {
          // id must have been evicted, so silently drop the deletion
        }

      case Evict(x) =>
        val (id, b) = key(x)
        if (state.contains(id)) {
          sendToParents(Evict(Tuple2(id, state(id))))
          state -= id
        }
    }
  }

  override def query(): Seq[(Id, B)] = {
    state.toSeq
  }
}

case class Join[A, B, C, D, E](
  keyLeft: A => (Id, B),
  keyRight: C => (Id, D),
  combine: (Id, B, D) => E,
  filterLeft: (Id, A) => Boolean,
  filterRight: (Id, C) => Boolean,
  left: Node[A],
  right: Node[C]) extends BinaryNode[A, C, E] {

  private val state = new mutable.HashMap[Id, (mutable.HashSet[B], mutable.HashSet[D])]

  override def query(): Seq[E] = {
    for {
      (id, (bs, ds)) <- state.toSeq
      b <- bs
      d <- ds
    } yield combine(id, b, d)
  }

  override def handleLeft(msg: Msg[A]): Unit = {
    println("Join.handleLeft " + msg)
    msg match {
      case Insert(x) =>
        val (id, b) = keyLeft(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          bs += b

          for (d <- ds) {
            sendToParents(Insert(combine(id, b, d)))
          }
        } else {
          val bs = mutable.HashSet[B]()
          bs += b
          state.update(id, (bs, mutable.HashSet()))
          for (c <- right.query(); if filterRight(id, c)) {
            handleRight(Insert(c))
          }
        }

      case Update(x) =>
        val (id, b) = keyLeft(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          if (bs.contains(b)) {
            bs -= b
            bs += b
            for (d <- ds) {
              sendToParents(Update(combine(id, b, d)))
            }
          } else {
            throw new Exception("got an update to a nonexistent record")
          }
        } else {
          // id must have been evicted, so silently drop the update
        }

      case Delete(x) =>
        val (id, b) = keyLeft(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          if (bs.contains(b)) {
            bs -= b
            for (d <- ds) {
              sendToParents(Delete(combine(id, b, d)))
            }
          } else {
            throw new Exception("got a delete to a nonexistent record")
          }
        } else {
          // id must have been evicted, so silently drop the delete
        }

      case Evict(x) =>
        val (id, _) = keyLeft(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          for (b <- bs; d <- ds) {
            sendToParents(Evict(combine(id, b, d)))
          }
          state -= id
        }
    }
  }

  override def handleRight(msg: Msg[C]): Unit = {
    println("Join.handleRight " + msg)
    msg match {
      case Insert(x) =>
        val (id, d) = keyRight(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          ds += d

          for (b <- bs) {
            sendToParents(Insert(combine(id, b, d)))
          }
        } else {
          val ds = mutable.HashSet[D]()
          ds += d
          state.update(id, (mutable.HashSet(), ds))
          for (a <- left.query(); if filterLeft(id, a)) {
            handleLeft(Insert(a))
          }
        }

      case Update(x) =>
        val (id, d) = keyRight(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          if (ds.contains(d)) {
            ds -= d
            ds += d
            for (b <- bs) {
              sendToParents(Update(combine(id, b, d)))
            }
          } else {
            ds += d
            for (b <- bs) {
              sendToParents(Update(combine(id, b, d)))
            }
          }
        } else {
          // id must have been evicted, so silently drop the update
        }

      case Delete(x) =>
        val (id, d) = keyRight(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          if (ds.contains(d)) {
            ds -= d
            for (b <- bs) {
              sendToParents(Delete(combine(id, b, d)))
            }
          } else {
            throw new Exception("got a delete to a nonexistent record")
          }
        } else {
          // id must have been evicted, so silently drop the delete
        }

      case Evict(x) =>
        val (id, _) = keyRight(x)
        if (state.contains(id)) {
          val (bs, ds) = state(id)
          for (b <- bs; d <- ds) {
            sendToParents(Evict(combine(id, b, d)))
          }
          state -= id
        }
    }
  }
}

case class TopK[A : Ordering](
  k: Int,
  child: Node[A]) extends UnaryNode[A, A] {

  private val state = mutable.SortedSet[A]()

  override def query(): Seq[A] = {
    state.toSeq
  }

  override def handle(msg: Msg[A]): Unit = {
    println("TopK.handle " + msg)
    msg match {
      case Insert(x) =>
        if (state.size < k) {
          state += x
          sendToParents(Insert(x))
        } else if (implicitly[Ordering[A]].gt(x, state.min)) {
          val dropped = state.min
          state -= dropped
          state += x
          sendToParents(Delete(dropped))
          sendToParents(Insert(x))
        }

      case Update(x) =>
        if (state.contains(x)) {
          state -= x
        }
        if (implicitly[Ordering[A]].gt(x, state.min)) {
          // The element remains in the top k after the update
          handle(Insert(x))
          sendToParents(Update(x))
        } else {
          // The update has caused this record to drop below the current minimum. It may still be in
          // the top k, or some other element may now replace it in the top k. For correctness we
          // must request a full refresh from the child.
          for (a <- query()) {
            sendToParents(Delete(a))
          }
          state.clear()
          for (a <- child.query()) {
            handle(Insert(a))
          }
        }

      case Delete(x) =>
        if (state.contains(x)) {
          state -= x
          sendToParents(Delete(x))

          // After the deletion, some other element may now replace the deleted element in the top
          // k. For correctness we must request a full refresh from the child.
          for (a <- child.query()) {
            handle(Insert(a))
          }
        }

      case Evict(x) =>
        // TopK does not support eviction because it inherently uses bounded space. Drop the
        // eviction request.
    }
  }
}

/**
 * Noria dataflow message.
 */
sealed trait Msg[A]

case class Insert[A](x: A) extends Msg[A]

case class Update[A](x: A) extends Msg[A]

case class Delete[A](x: A) extends Msg[A]

case class Evict[A](x: A) extends Msg[A]

// Application-specific record types

case class Vote(storyId: Id, voterId: Id, vote: Int)

case class Story(id: Id, submitterId: Id, title: String)

case class StoryWithVoteCount(id: Id, submitterId: Id, title: String, voteCount: Int)

object Noria {
  def main(args: Array[String]): Unit = {
    val votes = Table[Vote]()
    val stories = Table[Story]()
    val totalVotesByStoryId = Aggregate[Vote, Int](
      (vote: Vote) => (vote.storyId, 1),
      _ + _,
      _ - _,
      votes)
    val storiesWithVoteCount = Join[Story, Story, (Id, Int), Int, StoryWithVoteCount](
      (story: Story) => (story.id, story),
      (pair: (Id, Int)) => pair,
      (storyId, story: Story, voteCount: Int) => StoryWithVoteCount(
        story.id, story.submitterId, story.title, voteCount),
      (storyId, story: Story) => story.id == storyId,
      (storyId, pair: (Id, Int)) => pair match { case (storyId2, _) => storyId == storyId2 },
      stories,
      totalVotesByStoryId)
    val topStories =
      TopK(5, storiesWithVoteCount)(Ordering.by(_.voteCount))

    votes.addParent(totalVotesByStoryId)
    stories.addLeftParent(storiesWithVoteCount)
    totalVotesByStoryId.addRightParent(storiesWithVoteCount)
    storiesWithVoteCount.addParent(topStories)

    stories.handle(Insert(Story(1, 1, "Story 1")))
    stories.handle(Insert(Story(2, 1, "Story 2")))
    stories.handle(Insert(Story(3, 2, "Story 3")))

    println(topStories.query())

    votes.handle(Insert(Vote(1, 1, +1)))
    votes.handle(Insert(Vote(1, 2, +1)))
    votes.handle(Insert(Vote(1, 3, +1)))
    votes.handle(Insert(Vote(2, 1, -1)))
    votes.handle(Insert(Vote(2, 2, +1)))
    votes.handle(Insert(Vote(2, 3, +1)))

    println(topStories.query())
  }
}
