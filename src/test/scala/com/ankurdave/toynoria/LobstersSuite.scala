package com.ankurdave.toynoria

import org.scalatest.FunSuite

import com.ankurdave.toynoria.examples._

class LobstersSuite extends FunSuite {
  test("top stories") {
    val l = new Lobsters()

    logTrace("Inserting stories")
    l.stories.handle(Insert(Story(1, "Story A")))
    l.stories.handle(Insert(Story(2, "Story B")))
    l.stories.handle(Insert(Story(3, "Story C")))

    logTrace("Inserting 3 upvotes for story 1")
    l.votes.handle(Insert(Vote(1, +1)))
    l.votes.handle(Insert(Vote(1, +1)))
    l.votes.handle(Insert(Vote(1, +1)))

    logTrace("Inserting 1 downvote for story 2")
    l.votes.handle(Insert(Vote(2, -1)))

    assert(l.topStories.query() ===
      Seq(
        StoryWithVoteCount(2, "Story B", -1),
        StoryWithVoteCount(1, "Story A", 3)))

    l.votes.handle(Insert(Vote(2, +1)))
    l.votes.handle(Insert(Vote(2, +1)))
    l.votes.handle(Insert(Vote(3, +100)))

    assert(l.topStories.query() ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 100)))

    l.stories.handle(Evict(Story(1, "Story A")))
    l.stories.handle(Evict(Story(2, "Story B")))
    l.stories.handle(Evict(Story(3, "Story C")))

    l.votes.handle(Insert(Vote(3, +1)))

    assert(l.topStories.query() ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 101)))

    l.votes.handle(Evict(Vote(1, 0)))
    l.votes.handle(Evict(Vote(2, 0)))
    l.votes.handle(Evict(Vote(3, 0)))

    l.votes.handle(Insert(Vote(3, +1)))

    assert(l.topStories.query() ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 102)))
  }
}
