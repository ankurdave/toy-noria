package com.ankurdave.toynoria

import org.scalatest.FunSuite

case class Vote(override val id: Id, vote: Int) extends Record

case class Story(override val id: Id, title: String) extends Record

case class StoryWithVoteCount(override val id: Id, title: String, voteCount: Int) extends Record

class LobstersSuite extends FunSuite {
  test("top stories") {
    val votes = Table[Vote]()
    val stories = Table[Story]()
    val totalVotesByStoryId = Aggregate[Vote](
      id => Vote(id, 0),
      (a, b) => {
        assert(a.id == b.id)
        Vote(a.id, a.vote + b.vote)
      },
      (a, b) => {
        assert(a.id == b.id)
        Vote(a.id, a.vote - b.vote)
      },
      votes)
    val storiesWithVoteCount = Join[Story, Vote, StoryWithVoteCount](
      (story: Story, voteCount: Vote) => {
        assert(story.id == voteCount.id)
        StoryWithVoteCount(story.id, story.title, voteCount.vote)
      },
      stories,
      totalVotesByStoryId)
    val topStories =
      TopK(2, storiesWithVoteCount)(Ordering.by(_.voteCount))

    votes.addParent(totalVotesByStoryId)
    stories.addLeftParent(storiesWithVoteCount)
    totalVotesByStoryId.addRightParent(storiesWithVoteCount)
    storiesWithVoteCount.addParent(topStories)
    // Sec 4.5: Noria disables partial state for operators upstream of full-state descendants. TopK
    // is a full-state descendant of Aggregate.
    totalVotesByStoryId.disablePartialState()

    logTrace("Inserting stories")
    stories.handle(Insert(Story(1, "Story A")))
    stories.handle(Insert(Story(2, "Story B")))
    stories.handle(Insert(Story(3, "Story C")))

    logTrace("Inserting 3 upvotes for story 1")
    votes.handle(Insert(Vote(1, +1)))
    votes.handle(Insert(Vote(1, +1)))
    votes.handle(Insert(Vote(1, +1)))

    logTrace("Inserting 1 downvote for story 2")
    votes.handle(Insert(Vote(2, -1)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(2, "Story B", -1),
        StoryWithVoteCount(1, "Story A", 3)).map(_.toString))

    votes.handle(Insert(Vote(2, +1)))
    votes.handle(Insert(Vote(2, +1)))
    votes.handle(Insert(Vote(3, +100)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 100)).map(_.toString))

    stories.handle(Evict(Story(1, "Story A")))
    stories.handle(Evict(Story(2, "Story B")))
    stories.handle(Evict(Story(3, "Story C")))

    votes.handle(Insert(Vote(3, +1)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 101)).map(_.toString))

    votes.handle(Evict(Vote(1, 0)))
    votes.handle(Evict(Vote(2, 0)))
    votes.handle(Evict(Vote(3, 0)))

    votes.handle(Insert(Vote(3, +1)))

    assert(topStories.query().map(_.toString) ===
      Seq(
        StoryWithVoteCount(1, "Story A", 3),
        StoryWithVoteCount(3, "Story C", 102)).map(_.toString))
  }
}
